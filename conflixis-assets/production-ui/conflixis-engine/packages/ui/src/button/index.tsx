import { ReactNode } from 'react';
import classNames from 'classnames';
import { Tooltip } from '@material-tailwind/react';
import { Loader } from '../loader';

type Props = {
  text: string;
  className?: string;
  disabled?: boolean;
  showTooltip?: boolean;
  tooltipText?: string;
  tooltipPosition?: 'top' | 'bottom' | 'left' | 'right';
  type?: 'button' | 'submit' | 'reset';
  showLoader?: boolean;
  icon?: ReactNode;
  iconAfter?: ReactNode;
  formId?: string;
  style?: 'primary' | 'primary-outlined' | 'secondary' | 'link' | 'white' | 'green' | 'danger';
  onClick?(values?: any): void;
};

export const Button = ({
  text,
  className = '',
  disabled = false,
  showTooltip = false,
  tooltipText,
  tooltipPosition = 'top',
  type = 'button',
  showLoader = false,
  icon,
  iconAfter,
  formId = '',
  style = 'primary',
  onClick,
}: Props) => {
  const isDisabled = showLoader || disabled;
  const classes = classNames('!py-2 !px-4 !rounded-md font-soehneLight', {
    'bg-blue-500 hover:bg-blue-700 text-white': style === 'primary' && !isDisabled,
    'bg-white text-blue-500 border !border-blue-400 hover:!bg-blue-50':
      style === 'primary-outlined' && !disabled,

    'bg-white text-blue-500 border !border-blue-400 cursor-auto opacity-50':
      style === 'primary-outlined' && disabled,

    'bg-blue-500 text-white cursor-auto opacity-50': style === 'primary' && isDisabled,

    'bg-conflixis-gray hover:bg-conflixis-gray-hover': style === 'secondary' && !isDisabled,

    'bg-conflixis-gray cursor-auto opacity-50': style === 'secondary' && isDisabled,

    'bg-conflixis-white text-gray-900 border border-gray-300 shadow-sm hover:bg-gray-50':
      style === 'white' && !isDisabled,
    'bg-gray-50 cursor-auto opacity-50 text-gray-900 border border-gray-300':
      style === 'white' && isDisabled,

    'bg-green-500 text-white hover:bg-green-600': style === 'green' && !isDisabled,
    'bg-green-500 text-white cursor-auto opacity-50': style === 'green' && isDisabled,

    'bg-red-500 text-white hover:bg-red-600': style === 'danger' && !isDisabled,
    'bg-red-500 text-white cursor-auto opacity-50': style === 'danger' && isDisabled,

    'underline-link': style === 'link',
    'flex items-center gap-2': icon || showLoader || iconAfter,
    [className]: className,
  });

  if (showTooltip) {
    return (
      <Tooltip
        placement={tooltipPosition}
        content={tooltipText}
        className="border rounded-md text-gray-700 border-blue-gray-50 bg-white px-4 py-3 shadow-sm shadow-gray-500/10"
      >
        <button onClick={onClick} form={formId} className={classes} type={type} disabled={disabled}>
          {icon}
          {showLoader && <Loader className="h-5 w-5 fill-white" />}
          {text}
          {iconAfter}
        </button>
      </Tooltip>
    );
  }

  return (
    <button
      onClick={isDisabled ? undefined : onClick}
      form={formId}
      className={classes}
      type={type}
      disabled={isDisabled}
    >
      {icon}
      {showLoader && <Loader className="h-5 w-5 fill-white flex" />}
      {text}
      {iconAfter}
    </button>
  );
};
